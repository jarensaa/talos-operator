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
	"slices"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	talosv1alpha1 "github.com/jarensaa/talos-operator/api/v1alpha1"
	"github.com/jarensaa/talos-operator/internal/plan"
	"github.com/jarensaa/talos-operator/internal/render"
	"github.com/jarensaa/talos-operator/internal/talos"
)

// The Kubernetes upgrade executor (docs/scope.md §7.4). An upgrade changes the image a node's
// config names for a component, one component on one node at a time, in the order
// plan.PlanKubernetes gives. Each change is the node's live config with that one image replaced,
// and on a control plane's kubelet the kube-proxy image too, which nothing runs from; it is
// applied the way every config change is: tried, verified from outside, confirmed. The only
// things added to a config step are the pull before it and one more check after it: that the
// component runs the new version.

// kubeSystem is where Talos's static pods and kube-proxy live.
const kubeSystem = "kube-system"

// The chart binds this in kube-system only (templates/rbac.yaml): it is read to say whether the
// kube-proxy Talos deployed at bootstrap was left behind.
// +kubebuilder:rbac:groups=apps,resources=daemonsets,verbs=get

// KubernetesChecker asks the Kubernetes API what stands in the way of an upgrade.
type KubernetesChecker interface {
	// RemovedAPIObjects describes the objects of APIs that the upgrade from Kubernetes from to to
	// stops serving.
	RemovedAPIObjects(ctx context.Context, from, to string) ([]string, error)
}

// KubernetesCheckerFunc is a function as a KubernetesChecker.
type KubernetesCheckerFunc func(ctx context.Context, from, to string) ([]string, error)

// RemovedAPIObjects implements KubernetesChecker.
func (f KubernetesCheckerFunc) RemovedAPIObjects(ctx context.Context, from, to string) ([]string, error) {
	return f(ctx, from, to)
}

// withLiveKubernetes is what a config rollout applies to a node: the rendered config, with the
// image of every Kubernetes component as the node's own config names it now. Those images are a
// Kubernetes upgrade's to change, in its own order (docs/scope.md §7.4), so they are never part
// of what a config rollout compares or applies. Everything else of a change goes through, also
// while a Kubernetes upgrade is due, in flight or refused: a refused upgrade must not hold up
// the config change that clears its way. It reports whether it changed anything.
func withLiveKubernetes(rendered *render.Config, liveRaw []byte) (*render.Config, bool, error) {
	live, err := talos.KubernetesImages(liveRaw)
	if err != nil {
		return nil, false, err
	}

	want, err := talos.KubernetesImages(rendered.Bytes)
	if err != nil {
		return nil, false, err
	}

	raw, kept := rendered.Bytes, false

	for _, component := range talos.KubernetesComponents {
		if live[component] == "" || want[component] == "" || live[component] == want[component] {
			continue
		}

		if raw, err = talos.WithKubernetesImage(raw, component, live[component]); err != nil {
			return nil, false, err
		}

		kept = true
	}

	if !kept {
		return rendered, false, nil
	}

	parsed, err := talos.ParseMachineConfig(raw)
	if err != nil {
		return nil, false, err
	}

	out, err := render.Normalize(parsed)

	return out, true, err
}

// kubernetesImages is what a machine's node and its rendered config say about the Kubernetes
// components the machine runs; nil if either cannot be told.
func kubernetesImages(v machineView) map[string]plan.KubernetesImage {
	if v.node == nil || v.live == nil || v.rendered == nil {
		return nil
	}

	live, err := talos.KubernetesImages(v.node.activeConfig)
	if err != nil {
		return nil
	}

	desired, err := talos.KubernetesImages(v.rendered.Bytes)
	if err != nil {
		return nil
	}

	images := map[string]plan.KubernetesImage{}

	for _, component := range plan.KubernetesComponents {
		l, d := live[component], desired[component]
		if l == "" && d == "" {
			continue
		}

		images[component] = plan.KubernetesImage{Live: l, Desired: d}
	}

	return images
}

// kubernetesMachines is the planner's view of every member.
func kubernetesMachines(ctx context.Context, f *fleet) ([]plan.KubernetesMachine, error) {
	planned := make([]plan.KubernetesMachine, 0, len(f.members))

	for i := range f.members {
		v, err := f.view(ctx, &f.members[i])
		if err != nil {
			return nil, err
		}

		m := plan.KubernetesMachine{Name: v.tm.Name, Node: v.hostname(), Hold: v.held(), ControlPlane: v.controlPlane()}
		m.Images = kubernetesImages(v)
		m.Current = m.Images != nil

		planned = append(planned, m)
	}

	return planned, nil
}

// planKubernetes owns KubernetesUpToDate while no operation is in flight: it asks the planner
// whether an upgrade is due, and starts it by taking the lock. Like planUpgrade, it reports
// whether it did and, when an upgrade is due but may not start, why.
func (r *TalosClusterReconciler) planKubernetes(ctx context.Context, tc *talosv1alpha1.TalosCluster, obs observation, f *fleet,
) (bool, *metav1.Condition, error) {
	st := &tc.Status

	planned, err := kubernetesMachines(ctx, f)
	if err != nil {
		return false, nil, err
	}

	if !tc.Spec.Management.MayUpgrade() {
		r.reportKubernetes(ctx, tc, plan.PlanKubernetes(planned, plan.KubernetesSettings{Releases: releasesOf(tc)}, passOf(obs)), planned, false)

		return false, nil, nil
	}

	// A version the nodes' Talos does not run may not even render. It is refused for what it is,
	// before anything is asked of a rendered config.
	if problem := unsupportedKubernetes(tc, obs); problem != "" {
		setCondition(st, tc.Generation, talosv1alpha1.ConditionKubernetesUpToDate, metav1.ConditionFalse,
			talosv1alpha1.ReasonIncompatible, problem)

		return false, &metav1.Condition{
			Type: talosv1alpha1.ConditionRolloutBlocked, Status: metav1.ConditionTrue, Reason: talosv1alpha1.ReasonIncompatible,
			Message: "the Kubernetes upgrade does not start: " + problem,
		}, nil
	}

	settings := plan.KubernetesSettings{
		Aborted: held(tc, talosv1alpha1.OperationKubernetesUpgrade).Aborted[talosv1alpha1.HoldEverything],
		Policy:  obs.policy, Releases: releasesOf(tc),
	}

	if hop := plan.HopOf(planned, settings.Releases); hop.Due() {
		settings.Problems, settings.Unchecked = r.checkKubernetes(ctx, obs, hop)
	}

	result := plan.PlanKubernetes(planned, settings, passOf(obs))

	// An abort covers one version. Once the spec asks for another, it is spent.
	if !result.AbortStands {
		hold(tc, talosv1alpha1.OperationKubernetesUpgrade, func(h *talosv1alpha1.Holds) { clear(h.Aborted) })
	}

	// Last, because it asks upstream: an image that certainly does not exist is refused before a
	// node is asked to pull it.
	if result.MayStart() {
		if problem := r.imagesProblem(ctx, result.Targets); problem != "" {
			result.BlockReason, result.BlockMessage = plan.BlockIncompatible, problem
		}
	}

	op := r.start(tc, talosv1alpha1.OperationKubernetesUpgrade, result.Due)
	if op != nil {
		op.Target = result.Hop.Version
	}

	return op != nil, r.reportKubernetes(ctx, tc, result, planned, true), nil
}

// talosVersions is the Talos version of every node that was read, by hostname, and the name of a
// node whose version was not.
func talosVersions(obs observation) (map[string]string, string) {
	versions := map[string]string{}

	for _, n := range obs.facts.nodes {
		if n.version == "" {
			return versions, n.hostname
		}

		versions[n.hostname] = n.version
	}

	return versions, ""
}

// unsupportedKubernetes says why the Kubernetes version the spec asks for cannot run on the
// nodes' Talos, if it cannot.
func unsupportedKubernetes(tc *talosv1alpha1.TalosCluster, obs observation) string {
	if tc.Spec.Versions == nil || tc.Spec.Versions.Kubernetes == "" {
		return ""
	}

	versions, _ := talosVersions(obs)

	return talos.KubernetesSupportProblem(tc.Spec.Versions.Kubernetes, versions)
}

// checkKubernetes runs the checks of a hop before any node is touched: that every node's Talos
// supports the version, that no component is started with something the version removed, and
// that no object of an API it stops serving exists. It returns what they found, and why they
// could not all be run, if they could not.
func (r *TalosClusterReconciler) checkKubernetes(ctx context.Context, obs observation, hop plan.KubernetesHop) ([]string, string) {
	versions, unread := talosVersions(obs)
	if unread != "" {
		return nil, "the Talos version of " + unread + " was not read"
	}

	if problem := talos.KubernetesUpgradeProblem(hop.From, hop.Version, versions); problem != "" {
		return []string{problem}, ""
	}

	nodes := talos.ClusterInfo{ControlPlanes: obs.controlPlanes, Workers: obs.workers}

	problems, err := obs.reader.KubernetesUpgradeChecks(ctx, hop.From, hop.Version, nodes)
	if err != nil {
		logf.FromContext(ctx).Info("checking the nodes for a Kubernetes upgrade failed", "error", err.Error())

		return nil, err.Error()
	}

	if r.KubernetesChecks == nil {
		return problems, "the operator was started without access to the Kubernetes API for it"
	}

	objects, err := r.KubernetesChecks.RemovedAPIObjects(ctx, hop.From, hop.Version)
	if err != nil {
		logf.FromContext(ctx).Info("checking the Kubernetes API for a Kubernetes upgrade failed", "error", err.Error())

		return problems, "the Kubernetes API could not be asked for objects of APIs the new version removes: " + err.Error()
	}

	return append(problems, objects...), ""
}

// reportKubernetes sets KubernetesUpToDate while no upgrade is in flight. It returns the condition
// that says an upgrade is due and held up, if it is.
func (r *TalosClusterReconciler) reportKubernetes(ctx context.Context, tc *talosv1alpha1.TalosCluster, result plan.KubernetesUpgrade,
	planned []plan.KubernetesMachine, enabled bool,
) *metav1.Condition {
	st, gen := &tc.Status, tc.Generation
	upToDate := func(status metav1.ConditionStatus, reason, format string, args ...any) {
		setCondition(st, gen, talosv1alpha1.ConditionKubernetesUpToDate, status, reason, fmt.Sprintf(format, args...))
	}

	var stale []string

	for _, m := range planned {
		if !m.Current {
			stale = append(stale, m.Name)
		}
	}

	behind := strings.Join(result.Behind, ", ")

	switch {
	case st.Operation != nil && st.Operation.Type == talosv1alpha1.OperationKubernetesUpgrade:
		upToDate(metav1.ConditionFalse, talosv1alpha1.ReasonUpgrading, "upgrading to %s: %s", result.Hop.Version,
			strings.Join(result.Pending, ", "))
	case len(result.Behind) > 0 && !enabled:
		upToDate(metav1.ConditionFalse, talosv1alpha1.ReasonUpgradesDisabled,
			"the config of %s names other Kubernetes versions than their spec asks for; "+
				"set spec.management.upgrades to Enabled to upgrade them", behind)
	case len(result.Pending) > 0:
		upToDate(metav1.ConditionFalse, result.BlockReason, "%s to be upgraded to Kubernetes %s: %s", behind, result.Hop.To, result.BlockMessage)

		// A machine whose status is catching up is a matter of seconds, not something to act on.
		if result.BlockReason != plan.BlockNotCurrent || len(stale) == 0 {
			return &metav1.Condition{
				Type: talosv1alpha1.ConditionRolloutBlocked, Status: metav1.ConditionTrue, Reason: result.BlockReason,
				Message: fmt.Sprintf("the Kubernetes upgrade of %s does not start: %s", behind, result.BlockMessage),
			}
		}
	case len(result.Held) > 0:
		upToDate(metav1.ConditionFalse, talosv1alpha1.ReasonHeld, "%s do not name the Kubernetes version their spec asks for, and are on hold",
			strings.Join(result.Held, ", "))
	case len(stale) > 0:
		upToDate(metav1.ConditionUnknown, talosv1alpha1.ReasonAwaitingStatus, "waiting for the config of %s", strings.Join(stale, ", "))
	default:
		upToDate(metav1.ConditionTrue, talosv1alpha1.ReasonSucceeded, "all %d nodes name the Kubernetes version their spec asks for%s",
			len(planned), r.proxyNote(ctx, renderedVersion(planned)))
	}

	return nil
}

// renderedVersion is the Kubernetes version every machine's rendered config names; empty if they
// differ or cannot be told.
func renderedVersion(planned []plan.KubernetesMachine) string {
	found := ""

	for _, m := range planned {
		for _, image := range m.Images {
			v, ok := plan.VersionOf(image.Desired)
			if !ok || (found != "" && found != v) {
				return ""
			}

			found = v
		}
	}

	return found
}

// proxyNote says that the kube-proxy Talos deployed when the cluster was bootstrapped runs
// another version than the cluster. The operator does not change it (docs/scope.md §12.20), so
// this is the one place its being left behind is said. Whatever cannot be read says nothing.
func (r *TalosClusterReconciler) proxyNote(ctx context.Context, version string) string {
	if version == "" {
		return ""
	}

	proxy := &appsv1.DaemonSet{}
	if err := r.apiReader().Get(ctx, client.ObjectKey{Namespace: kubeSystem, Name: "kube-proxy"}, proxy); err != nil {
		return ""
	}

	for _, c := range proxy.Spec.Template.Spec.Containers {
		if v, ok := plan.VersionOf(c.Image); ok && v != version {
			return fmt.Sprintf("; the kube-proxy DaemonSet runs %s, which the operator does not change", c.Image)
		}
	}

	return ""
}

// kubernetesTurns is what the turns of a Kubernetes upgrade do: one component on one machine each.
// A machine that is no longer a member has an empty view.
func (r *TalosClusterReconciler) kubernetesTurns(ctx context.Context, tc *talosv1alpha1.TalosCluster, obs observation, f *fleet,
	timing plan.Timing,
) turns {
	op := tc.Status.Operation

	view := func(key string) (machineView, error) {
		_, name := plan.SplitTurn(key)

		return f.view(ctx, f.member(name))
	}

	return turns{
		begin: func(key string, attempt int32) error {
			v, err := view(key)
			if err != nil {
				return err
			}

			planned, err := kubernetesMachines(ctx, f)
			if err != nil {
				return err
			}

			r.beginKubernetes(tc, key, v, plan.PlanKubernetes(planned, plan.KubernetesSettings{Releases: releasesOf(tc)}, passOf(obs)), attempt)

			return nil
		},
		step: func() (string, error) {
			v, err := view(op.Current.Machine)
			if err != nil {
				return "", err
			}

			return r.kubernetesStep(ctx, tc, obs, v, timing)
		},
		// The version whose upgrade a human ended is the one the spec asks for, so that no later
		// hop towards it starts either.
		abort: func() {
			version := op.Target

			if planned, err := kubernetesMachines(ctx, f); err == nil {
				if hop := plan.HopOf(planned, releasesOf(tc)); hop.To != "" {
					version = hop.To
				}
			}

			hold(tc, op.Type, func(h *talosv1alpha1.Holds) { h.Aborted[talosv1alpha1.HoldEverything] = version })
		},
	}
}

// beginKubernetes persists the first step of a turn: one component on one machine. Nothing is
// sent anywhere in the same reconcile. current is the upgrade as it would be planned now: a turn
// that is no longer part of it, or would get another image, ends the operation, and the rest is
// planned afresh.
func (r *TalosClusterReconciler) beginKubernetes(tc *talosv1alpha1.TalosCluster, key string, v machineView, current plan.KubernetesUpgrade,
	attempt int32,
) {
	op := tc.Status.Operation
	component, machine := plan.SplitTurn(key)
	target, want := op.Targets[key], plan.TurnImages(op.Targets, key)

	step := &talosv1alpha1.OperationStep{
		Machine:   machine,
		Component: component,
		Step:      plan.StepPulling,
		Image:     target,
		Attempt:   attempt,
		Since:     r.stamp(),
	}

	images := kubernetesImages(v)

	switch {
	case v.tm == nil:
		// The TalosMachine is gone, so there is nothing left to configure.
		skipped(op, key)
	case v.held() && v.controlPlane():
		r.end(tc, talosv1alpha1.OperationSuperseded, machine+" was put on hold, and a Kubernetes upgrade cannot leave a control plane out")
	case v.held():
		skipped(op, key)
	case images != nil && names(images, want):
		// Nothing to apply, but the upgrade only moves past a component it has seen run.
		step.Step, step.PreviousHash, step.DesiredHash = plan.StepVerifying, v.live.Hash, v.live.Hash
		op.Current = step
	case images != nil && !maps.Equal(plan.TurnImages(current.Targets, key), want):
		r.end(tc, talosv1alpha1.OperationSuperseded,
			fmt.Sprintf("%s on %s is to get another image than this upgrade planned", component, machine))
	default:
		// With the node not read, the turn begins anyway: its first step waits for the node, and
		// that wait is bounded.
		op.Current = step
	}
}

// names reports whether a node's config names every image a turn brings its components to.
func names(images map[string]plan.KubernetesImage, want map[string]string) bool {
	for component, image := range want {
		if images[component].Live != image {
			return false
		}
	}

	return true
}

// turnConfig is the node's config raw with every image the turn in flight changes replaced: the
// component's own, and what its turn brings along.
func turnConfig(raw []byte, op *talosv1alpha1.Operation) ([]byte, error) {
	cur := op.Current
	images := plan.TurnImages(op.Targets, plan.TurnKey(cur.Component, cur.Machine))

	for _, component := range plan.Changes(cur.Component) {
		image, ok := images[component]
		if !ok {
			continue
		}

		var err error
		if raw, err = talos.WithKubernetesImage(raw, component, image); err != nil {
			return nil, err
		}
	}

	return raw, nil
}

// kubernetesStep asks the planner what the turn in flight needs next, and does it. Once the image
// is on the node the turn is a config change like any other, with one more check: that the
// component runs the new version.
func (r *TalosClusterReconciler) kubernetesStep(ctx context.Context, tc *talosv1alpha1.TalosCluster, obs observation, v machineView,
	timing plan.Timing,
) (string, error) {
	op := tc.Status.Operation
	cur := op.Current
	now := r.now()
	s, node := stepOf(cur), stepNode(v)

	// A node nothing was sent to is left alone while the upgrade is held back. The step is
	// dropped; the turn begins again later.
	hold := obs.policy.Hold()
	if !hold.Open && (cur.Step == plan.StepPulling || plan.Untouched(s, node, now)) {
		op.Current = nil

		return hold.Reason, nil
	}

	pre := hold.And(plan.TouchGate(passOf(obs), v.hostname())).And(obs.policy.Gates())
	if v.tm == nil {
		pre = plan.Gate{Reason: "the TalosMachine no longer exists"}
	}

	checks, err := r.verified(ctx, obs, v.boundMachine, cur, node.Reachable, now)
	if err != nil {
		return "", err
	}

	if checks.Open {
		if checks, err = r.componentRuns(ctx, v.boundMachine, cur); err != nil {
			return "", err
		}
	}

	action := plan.NextKubernetesStep(s, node, pre, checks, timing, now)
	logAnswer(ctx, op, action, pre, "image", cur.Image, "reachable", node.Reachable, "activeHash", node.ActiveHash,
		"persistentHash", node.PersistentHash, "previousHash", cur.PreviousHash, "desiredHash", cur.DesiredHash)

	if note, done := answered(op, action, now); done {
		return note, nil
	}

	switch action.Kind {
	case plan.Rearm:
		deadline := metav1.NewTime(now.Add(plan.KubernetesTryTimeout(cur.Component, timing)))
		cur.Deadline = &deadline
	case plan.Pull:
		return r.pullComponent(ctx, tc, obs, v, timing, now)
	case plan.Try:
		raw, err := turnConfig(v.node.activeConfig, op)
		if err != nil {
			failOperation(op, plan.FailRejected, err.Error(), now)

			return "", nil
		}

		// The planner only asks for a try while the node runs the config the step started from, so
		// the patch gives the config that was recorded then. Anything else is not sent.
		if hash, _ := configHash(raw); hash != cur.DesiredHash {
			return "the node's config with the new images is not the config this step recorded", nil
		}

		return r.tryConfig(ctx, tc, obs, v, raw, now)
	case plan.Confirm:
		return r.confirm(ctx, tc, obs, v)
	case plan.Complete:
		// The node runs, persistently, a config the operator put there: its new baseline, as
		// after any config change (docs/scope.md §8.5.1).
		if err := r.recordApplied(ctx, v.tm, cur.DesiredHash); err != nil {
			return "", err
		}

		completed(tc)
	default:
		return "", unexpected(op, action)
	}

	return "", nil
}

// pullComponent pulls the component's image to the node, and then records the config change that
// follows: the node's config as it is now, and as it is with the turn's images replaced. A pull
// changes nothing on the node and can be repeated, so a pass that is interrupted simply does it
// again.
func (r *TalosClusterReconciler) pullComponent(ctx context.Context, tc *talosv1alpha1.TalosCluster, obs observation, v machineView,
	timing plan.Timing, now time.Time,
) (string, error) {
	op := tc.Status.Operation
	cur := op.Current

	// Asked for first, although an image the node's config names already is not pulled: what is
	// recorded below is only recorded from the TalosCluster as the API server has it now.
	upgrader, note := clientTo(ctx, r, tc, obs, "pull the image with", r.Resolver.ResolveUpgrader)
	if note != "" {
		return note, nil
	}

	raw, err := turnConfig(v.node.activeConfig, op)
	if err != nil {
		failOperation(op, plan.FailRejected, err.Error(), now)

		return "", nil
	}

	desired, err := configHash(raw)
	if err != nil {
		failOperation(op, plan.FailRejected, fmt.Sprintf("the config with the %s images replaced does not parse", cur.Component), now)

		return "", nil
	}

	if desired == v.live.Hash {
		// The config names the image already: only the component is left to be seen running.
		moveTo(cur, plan.StepVerifying, now)
		cur.PreviousHash, cur.DesiredHash = desired, desired

		return "", nil
	}

	// A turn that only brings the kube-proxy image along runs the image its component runs now,
	// which is on the node.
	if live, err := talos.KubernetesImages(v.node.activeConfig); err != nil || live[cur.Component] != cur.Image {
		// A download takes as long as it takes. The step's own timeout bounds it.
		callCtx, cancel := context.WithTimeout(ctx, timing.StepTimeout)
		defer cancel()

		if err := upgrader.PullKubernetesImage(callCtx, v.node.address, cur.Component, cur.Image); err != nil {
			if talos.IsUnreachable(err) {
				return retried(ctx, op, "reach the node", err), nil
			}

			failOperation(op, plan.FailImagePull, fmt.Sprintf("could not pull %s; nothing on the node changed: %v", cur.Image, err), now)

			return "", nil
		}
	}

	// The deadline is persisted before the try is sent, as in a config rollout.
	deadline := metav1.NewTime(now.Add(plan.KubernetesTryTimeout(cur.Component, timing)))

	moveTo(cur, plan.StepTrying, now)
	cur.PreviousHash, cur.DesiredHash, cur.Deadline = v.live.Hash, desired, &deadline

	return "", nil
}

// componentRuns checks that the component of the step in flight runs the image the step applied,
// and is ready: for the kubelet by the version its Node reports, for a static pod by the pod the
// kubelet mirrors into the API. kube-proxy is not run from a node's config, has no turn, and so
// nothing to be seen.
func (r *TalosClusterReconciler) componentRuns(ctx context.Context, m boundMachine, cur *talosv1alpha1.OperationStep) (plan.Gate, error) {
	closed := func(format string, args ...any) (plan.Gate, error) {
		return plan.Gate{Reason: fmt.Sprintf(format, args...)}, nil
	}

	name := m.kubernetesNode()

	if cur.Component == plan.ComponentKubelet {
		want, ok := plan.VersionOf(cur.Image)
		if !ok {
			return closed("the image %s names no version to look for", cur.Image)
		}

		node := &corev1.Node{}
		if err := r.Get(ctx, client.ObjectKey{Name: name}, node); err != nil {
			return plan.Gate{}, fmt.Errorf("%w: get node %s: %w", errKubernetes, name, err)
		}

		// A kubelet reports its version the way an image's tag names it.
		if got, ok := plan.VersionOf("kubelet:" + node.Status.NodeInfo.KubeletVersion); !ok || got != want {
			return closed("the kubelet of %s reports %s, not %s yet", name, node.Status.NodeInfo.KubeletVersion, want)
		}

		return plan.Gate{Open: true}, nil
	}

	var pods corev1.PodList

	// Uncached, like the pods an upgrade drains: the manager does not watch kube-system.
	if err := r.apiReader().List(ctx, &pods, client.InNamespace(kubeSystem), client.MatchingFields{"spec.nodeName": name},
		client.MatchingLabels{"k8s-app": cur.Component}); err != nil {
		return plan.Gate{}, fmt.Errorf("%w: list %s pods on node %s: %w", errKubernetes, cur.Component, name, err)
	}

	for _, pod := range pods.Items {
		runs := slices.ContainsFunc(pod.Spec.Containers, func(c corev1.Container) bool { return c.Image == cur.Image })
		if pod.DeletionTimestamp != nil || !runs {
			continue
		}

		for _, c := range pod.Status.Conditions {
			if c.Type == corev1.PodReady && c.Status == corev1.ConditionTrue {
				return plan.Gate{Open: true}, nil
			}
		}

		return closed("%s on %s runs %s and is not ready yet", cur.Component, name, cur.Image)
	}

	return closed("%s on %s does not run %s yet", cur.Component, name, cur.Image)
}

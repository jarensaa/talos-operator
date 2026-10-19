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

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	talosv1alpha1 "github.com/jarensaa/talos-operator/api/v1alpha1"
)

// manage runs while config is Manage (docs/scope.md §7.1 stage 4).
//
// The first reconcile after entering Manage is the handover: it must find that taking ownership
// changes nothing, by the proof's own gate and a fresh Talos dry run on every node, because the
// adoption proof stops being re-evaluated once the cluster is managed. Until a handover is
// verified the cluster is Degraded and rollouts are blocked; the check is repeated every
// reconcile, so a spec that renders the live config again lifts the block.
//
// Once handed over, the rollout executor takes it from there: it reports whether every node
// runs its rendered config and rolls out the ones that do not (rollout.go).
func (r *TalosClusterReconciler) manage(ctx context.Context, tc *talosv1alpha1.TalosCluster, obs observation) error {
	st := &tc.Status
	gen := tc.Generation

	pools, all, err := membersOf(ctx, r.Client, tc.Namespace, tc.Name)
	if err != nil {
		return err
	}

	f := newFleet(tc, r.Client, obs.facts, pools, all)
	reportInstallerBootable(tc, f)

	pass := passOf(obs)
	obs.pass = &pass

	if obs.facts == nil || obs.reader == nil {
		// The cluster was not reached, so nothing can be compared or planned. A node step in flight
		// still has deadlines to keep: it goes on, with its node counted as unreachable.
		if st.Operation != nil {
			return r.carryOn(ctx, tc, obs, f)
		}

		return nil
	}

	if !meta.IsStatusConditionTrue(st.Conditions, talosv1alpha1.ConditionConfigHandover) {
		nodes, why := proofGate(tc, obs.facts, obs.unmatched, pools, f.members)
		problem := &configProblem{metav1.ConditionFalse, talosv1alpha1.ReasonHandoverUnverified, why}

		if why == "" {
			if problem, err = r.verifyNoChanges(ctx, tc, obs.reader, nodes); err != nil {
				return err
			}
		}

		if problem != nil {
			setCondition(st, gen, talosv1alpha1.ConditionConfigHandover, problem.status, problem.reason, problem.msg)
			setCondition(st, gen, talosv1alpha1.ConditionRolloutBlocked, metav1.ConditionTrue, talosv1alpha1.ReasonHandoverUnverified,
				"entering config: Manage is not verified to change nothing; no rollout starts until every node's rendered config "+
					"equals its live config again, or config returns to Observe: "+problem.msg)
			setCondition(st, gen, talosv1alpha1.ConditionConfigInSync, metav1.ConditionFalse, problem.reason, problem.msg)
			st.Phase = talosv1alpha1.ClusterPhaseDegraded

			return nil //nolint:nilerr // err is nil here: a problem is a verdict, reported in ConfigHandover
		}

		setCondition(st, gen, talosv1alpha1.ConditionConfigHandover, metav1.ConditionTrue, talosv1alpha1.ReasonSucceeded,
			fmt.Sprintf("taking ownership changes nothing: Talos's dry run reports no changes on all %d nodes", len(nodes)))
		meta.RemoveStatusCondition(&st.Conditions, talosv1alpha1.ConditionRolloutBlocked)
	}

	return r.operate(ctx, tc, obs, f)
}

// operate carries the one disruptive operation of the cluster a step further, or plans the next
// (docs/scope.md §8.1, invariant 1). The upgrades come before a config rollout: a Talos upgrade
// brings a node to the system its config names, a Kubernetes upgrade changes the images of the
// Kubernetes components in its own order, and the rollout then applies what is left.
//
// An upgrade that is due but may not start does not keep the other from starting. They wait for
// each other where they must: a Talos version that does not run the cluster's Kubernetes waits
// for the Kubernetes upgrade, and the other way round (docs/scope.md §7.3, §7.4).
//
// Machines leave before anything else is considered, and join after everything else (docs/scope.md
// §7.6, §7.7). A node that is to go is not upgraded first, and one that ails is taken out without
// waiting for a rollout it would hold up. A machine that is added joins a cluster that runs what
// its spec asks for. A replacement falls out of the two: the removal of a control plane that
// would leave too few is refused, the new one is added, and the old one then goes.
//
// Etcd is defragmented after everything else: a cluster that has something to roll out does that
// first (docs/scope.md §14.6).
func (r *TalosClusterReconciler) operate(ctx context.Context, tc *talosv1alpha1.TalosCluster, obs observation, f *fleet) error {
	if tc.Status.Operation != nil {
		err := r.carryOn(ctx, tc, obs, f)

		reportMembership(tc, f)

		return err
	}

	removing, held := r.planRemoval(tc, obs, f)
	if removing {
		return nil
	}

	upgrading, heldUpgrade := r.planUpgrade(ctx, tc, obs, f)
	if upgrading {
		return nil
	}

	upgrading, heldKubernetes, err := r.planKubernetes(ctx, tc, obs, f)
	if err != nil || upgrading {
		return err
	}

	if err := r.planRollout(ctx, tc, obs, f); err != nil {
		return err
	}

	// A removal or an upgrade that is due and may not start is what really holds things up: the
	// config that names an upgrade's installer waits for it.
	for _, cond := range []*metav1.Condition{held, heldUpgrade, heldKubernetes} {
		if cond != nil && tc.Status.Operation == nil {
			setCondition(&tc.Status, tc.Generation, cond.Type, cond.Status, cond.Reason, cond.Message)

			break
		}
	}

	if tc.Status.Operation == nil {
		if err := r.planJoin(ctx, tc, obs, f); err != nil {
			return err
		}
	}

	reportMembership(tc, f)

	if tc.Status.Operation == nil {
		r.planDefragment(ctx, tc, obs, f)
	}

	return nil
}

// forgetHandover drops the handover state when config returns to Observe, so entering Manage
// again is verified again.
func forgetHandover(st *talosv1alpha1.TalosClusterStatus) {
	meta.RemoveStatusCondition(&st.Conditions, talosv1alpha1.ConditionConfigHandover)
	meta.RemoveStatusCondition(&st.Conditions, talosv1alpha1.ConditionConfigInSync)
	// It is worked out only while the cluster is managed, and would otherwise be left looking current.
	meta.RemoveStatusCondition(&st.Conditions, talosv1alpha1.ConditionInstallerBootable)

	blocked := meta.FindStatusCondition(st.Conditions, talosv1alpha1.ConditionRolloutBlocked)
	if blocked != nil && blocked.Reason == talosv1alpha1.ReasonHandoverUnverified {
		meta.RemoveStatusCondition(&st.Conditions, talosv1alpha1.ConditionRolloutBlocked)
	}
}

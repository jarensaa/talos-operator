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

// managing reports whether the operator owns the cluster's config.
func managing(tc *talosv1alpha1.TalosCluster) bool {
	return tc.Spec.Management.Config == talosv1alpha1.ConfigManagementManage
}

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

	// What is rolled out to, upgraded and proven are the cluster's members. A machine that is
	// declared and has not joined, or has left, is the business of adding and removing alone.
	machines := members(all)

	if obs.facts == nil || obs.reader == nil {
		// The cluster was not reached, so nothing can be compared or planned. A node step in flight
		// still has deadlines to keep: it goes on, with its node counted as unreachable.
		if st.Operation != nil {
			return r.operate(ctx, tc, obs, pools, machines, all)
		}

		return nil
	}

	if !meta.IsStatusConditionTrue(st.Conditions, talosv1alpha1.ConditionConfigHandover) {
		nodes, why := proofGate(tc, obs.facts, obs.unmatched, pools, machines)
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

	return r.operate(ctx, tc, obs, pools, machines, all)
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
func (r *TalosClusterReconciler) operate(ctx context.Context, tc *talosv1alpha1.TalosCluster, obs observation,
	pools map[string]*talosv1alpha1.TalosMachinePool, machines, all []talosv1alpha1.TalosMachine,
) error {
	removing, held, err := r.removal(ctx, tc, obs, pools, all)
	if err != nil || removing {
		return err
	}

	upgrading, heldUpgrade, err := r.upgrade(ctx, tc, obs, pools, machines)
	if err != nil || upgrading {
		return err
	}

	upgrading, heldKubernetes, err := r.kubernetes(ctx, tc, obs, pools, machines)
	if err != nil || upgrading {
		return err
	}

	if err := r.rollout(ctx, tc, obs, pools, machines); err != nil {
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

	if err := r.join(ctx, tc, obs, pools, all); err != nil {
		return err
	}

	reportMembership(tc, obs, all)

	return nil
}

// forgetHandover drops the handover state when config returns to Observe, so entering Manage
// again is verified again.
func forgetHandover(st *talosv1alpha1.TalosClusterStatus) {
	meta.RemoveStatusCondition(&st.Conditions, talosv1alpha1.ConditionConfigHandover)
	meta.RemoveStatusCondition(&st.Conditions, talosv1alpha1.ConditionConfigInSync)

	blocked := meta.FindStatusCondition(st.Conditions, talosv1alpha1.ConditionRolloutBlocked)
	if blocked != nil && blocked.Reason == talosv1alpha1.ReasonHandoverUnverified {
		meta.RemoveStatusCondition(&st.Conditions, talosv1alpha1.ConditionRolloutBlocked)
	}
}
